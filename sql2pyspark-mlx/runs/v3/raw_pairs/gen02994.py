from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Toky%"))
    .select("city", F.lit("supplier").alias("source"))
    .unionByName(
        df_departments
        .filter(F.col("location") == "London")
        .select(F.col("location").alias("city"), F.lit("department").alias("source"))
    )
)
