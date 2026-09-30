from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc_nulls_first(), "id")

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-03-27"))) & F.col("segment").isNotNull())
    .select(
        "id",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.lower("name").alias("name_lower"),
        F.coalesce("segment", F.lit("N/A")).alias("resolved_segment"),
    )
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
