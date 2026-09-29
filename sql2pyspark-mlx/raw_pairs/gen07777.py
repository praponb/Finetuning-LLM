from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").asc_nulls_first(), "id")

valid = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-09-16"))) & F.month("sale_date").isin([8, 9, 12]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
