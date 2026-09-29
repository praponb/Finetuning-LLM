from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue_per_unit").desc_nulls_first())
w2 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("sale_date", "id")
w4 = Window.partitionBy("store_id").orderBy("least_revenue_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-04-03").cast("date"))
    .withColumn("revenue_per_unit", (F.col("revenue") * F.col("units")))
    .withColumn("least_revenue_units", F.least("revenue", "units"))
)

result = (
    valid
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "revenue_per_unit",
        "least_revenue_units",
        F.dense_rank().over(w).alias("revenue_per_unit_dense_rank"),
        F.avg("revenue_per_unit").over(w2).alias("revenue_per_unit_avg_6"),
        F.lead("least_revenue_units", 1).over(w3).alias("next_least_revenue_units"),
        F.last("least_revenue_units", ignorenulls=True).over(w4).alias("last_known_least_revenue_units"),
        F.corr("units", "revenue").over(w5).alias("corr_units_revenue"),
    )
)
