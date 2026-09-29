from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", "city", F.lit("customer").alias("source"))
    .unionByName(
        df_suppliers
        .filter(F.col("city").isin(["Singapore", "New York", "Bangkok"]))
        .select("country", "city", F.lit("supplier").alias("source"))
    )
    .distinct()
    .orderBy(F.col("city").desc())
)
