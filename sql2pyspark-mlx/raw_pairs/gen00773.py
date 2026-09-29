from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "London")
    .select(F.col("email").alias("label"))
    .intersect(
        df_suppliers
        .filter(F.col("country").isin(["UK", "TH", "US"]))
        .select(F.col("country").alias("label"))
    )
    .orderBy(F.col("label").desc())
)
