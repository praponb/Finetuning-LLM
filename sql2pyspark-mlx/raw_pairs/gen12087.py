from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "TH")
    .select("signup_date", "segment")
    .unionByName(
        df_customers
        .filter(~F.col("segment").isin(["Corporate", "Consumer"]))
        .select("signup_date", "segment")
    )
    .orderBy(F.col("signup_date").desc())
)
