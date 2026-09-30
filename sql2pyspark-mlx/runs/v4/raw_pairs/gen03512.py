from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").desc_nulls_last(), "id")

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2024-04-14").cast("date")) & F.col("segment").isNotNull() & F.col("credit_limit").between(9300, 15100))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        F.when(F.col("country").isin(["UK", "JP"]), "Tier 1").otherwise("Other").alias("country_group"),
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

result = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
