from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last(), "id")

ranked = (
    df_suppliers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "country", "city", "rating")
)

result = (
    ranked
    .select(
        "id",
        "country",
        "rating",
        F.when(F.col("country").rlike("^(TH|DE)$"), "PRIORITY").otherwise("CORE").alias("country_zone"),
    )
)
