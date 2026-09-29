from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

latest = (
    df_suppliers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "city",
        "rating",
        "dedup_rank",
        F.when(F.col("city").rlike("^(Berlin|New York)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").asc_nulls_last())
)
