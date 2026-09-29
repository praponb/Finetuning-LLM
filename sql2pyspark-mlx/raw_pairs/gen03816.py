from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").asc_nulls_first())

deduped = (
    df_customers
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "name", "country", "city", "segment", "is_active", "credit_limit")
)

result = (
    deduped
    .select(
        "id",
        "name",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        F.when(F.col("name").rlike("^(Jack Taylor|  Carol Diaz)$"), "GROUP_B").when(F.col("name").rlike("^(Alice Wong|bob smith)$"), "PRIORITY").otherwise("GROUP_A").alias("name_zone"),
    )
    .orderBy(F.col("credit_limit").desc_nulls_last())
)
