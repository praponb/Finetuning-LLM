from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") >= 58)
    .select(
        "id",
        "age",
        "email",
        "name",
        F.trim("country").alias("clean_country"),
        F.when(F.col("segment") == "SMB", 1).otherwise(0).alias("is_smb"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
