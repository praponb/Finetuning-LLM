from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age_filled").desc(), "id")

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull() & (F.col("city") != "Chiang Mai"))
    .select(
        "id",
        "name",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.length("name").alias("name_length"),
        F.coalesce("age", F.lit(0)).alias("age_filled"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
