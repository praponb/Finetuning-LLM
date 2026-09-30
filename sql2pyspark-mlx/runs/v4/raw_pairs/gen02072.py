from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper("name").like("%ACK%"))
    .select("name", "segment")
    .unionByName(
        df_customers
        .filter(F.col("email").like("%@gmail.com"))
        .select("name", "segment")
    )
    .distinct()
    .orderBy(F.col("name").desc())
)
