from pyspark.sql import functions as F

valid = (
    df_customers
    .filter(F.length("email") > 6)
    .select(
        "*",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

result = (
    valid
    .groupBy("city")
    .pivot("segment", ["Corporate", "SMB", "Consumer"])
    .agg(F.sum("credit_limit").alias("total"))
    .select(
        "city",
        F.col("Corporate").alias("corporate"),
        F.col("SMB").alias("smb"),
        F.col("Consumer").alias("consumer"),
    )
)
