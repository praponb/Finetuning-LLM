from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") != "DE")
    .select("email")
    .unionByName(
        df_customers
        .filter(F.col("country").isin(["UK", "TH", "DE"]))
        .select("email")
    )
    .distinct()
    .orderBy(F.col("email").desc())
)
