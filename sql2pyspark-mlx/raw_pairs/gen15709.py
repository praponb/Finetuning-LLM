from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_first())

ranked = (
    df_suppliers
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "country",
        "city",
        "rating",
        "latest_rank",
        F.when(F.col("city").rlike("^(Chiang Mai|New York)$"), "GROUP_B").when(F.col("city").rlike("^(Bangkok|Singapore)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy("city", "id")
)
