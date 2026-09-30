from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("years_since_sale_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("years_since_sale_date").desc_nulls_first())
w4 = Window.partitionBy("store_id").orderBy("years_since_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-08-14").cast("date"))
    .select(
        "*",
        (F.datediff(F.current_date(), "sale_date") / 365.25).alias("years_since_sale_date"),
    )
)

result = (
    filtered
    .filter(F.col("product_id") == 2)
    .select(
        "id",
        "store_id",
        "product_id",
        "years_since_sale_date",
        F.avg("years_since_sale_date").over(w).alias("years_since_sale_date_avg_5_prior"),
        F.corr("revenue", "units").over(w2).alias("corr_revenue_units"),
        F.rank().over(w3).alias("years_since_sale_date_rank"),
        F.last("years_since_sale_date", ignorenulls=True).over(w4).alias("last_known_years_since_sale_date"),
    )
)
