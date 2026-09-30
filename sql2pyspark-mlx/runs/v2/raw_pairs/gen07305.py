from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "JP") & F.col("email").isNotNull() & F.col("email").like("%@mail.co.th"))
    .select(
        "id",
        "country",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
    .orderBy("id", F.col("is_active_label").desc())
)
