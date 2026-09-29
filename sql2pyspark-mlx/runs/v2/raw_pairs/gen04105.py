from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", F.lit("customer").alias("source"))
    .unionByName(
        df_sessions
        .filter(F.col("country").isin(["TH", "JP"]))
        .select("country", F.lit("session").alias("source"))
    )
    .orderBy("country")
)
