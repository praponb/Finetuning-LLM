from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_first(), "id")

base = (
    df_store_sales
    .filter(F.col("units") == 22)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
