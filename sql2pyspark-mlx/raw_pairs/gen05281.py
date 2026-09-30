from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("adjusted_revenue_per_unit").desc(), "id")
w_part = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("rank_in_group", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("revenue") <= 2700)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)

valid = (
    filtered
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-02-14")))
    .select(
        "id",
        "product_id",
        "units",
        "revenue_per_unit",
        F.round((F.col("revenue_per_unit") * 1.1), 2).alias("adjusted_revenue_per_unit"),
    )
)

cleaned = (
    valid
    .filter(F.col("units") <= 21)
    .withColumn("adjusted_revenue_per_unit_int", F.col("adjusted_revenue_per_unit").cast("int"))
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "product_id",
        "units",
        "revenue_per_unit",
        F.max("adjusted_revenue_per_unit").over(w_part).alias("running_max_adjusted_revenue_per_unit"),
        F.avg("adjusted_revenue_per_unit").over(w_part).alias("running_avg_adjusted_revenue_per_unit"),
        F.count("adjusted_revenue_per_unit").over(w_part).alias("running_count_adjusted_revenue_per_unit"),
        F.avg("adjusted_revenue_per_unit").over(w3).alias("adjusted_revenue_per_unit_avg_6"),
    )
)
