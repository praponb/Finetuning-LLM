from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Chiang Mai")
    .select("segment", "name")
    .subtract(
        df_customers
        .filter(F.col("email").like("%@gmail.com"))
        .select("segment", "name")
    )
    .orderBy(F.col("segment").desc())
)
