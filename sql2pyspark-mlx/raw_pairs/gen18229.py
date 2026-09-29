from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("YEAR", "sale_date"), "product_id").orderBy("units_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_first())
w3 = Window.partitionBy(F.year("sale_date")).orderBy(F.col("sale_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-05-01").cast("date"))
    .select(
        "*",
        F.coalesce("units", F.lit(0)).alias("units_filled"),
        F.date_trunc("week", "sale_date").alias("sale_date_period"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "sale_date_period",
        F.last("revenue", ignorenulls=True).over(w).alias("last_known_revenue"),
        F.dense_rank().over(w2).alias("units_dense_rank"),
        F.avg("units_filled").over(w3).alias("units_filled_avg_90d"),
        F.sum("revenue").over(w4).alias("cumulative_revenue"),
    )
)
