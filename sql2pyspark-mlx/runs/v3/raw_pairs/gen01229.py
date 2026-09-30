from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "JP"]) & F.lower("supplier_name").like("%iam%"))
    .agg(
        F.count("rating").alias("rating_count"),
        F.count("*").alias("num_suppliers"),
    )
)
