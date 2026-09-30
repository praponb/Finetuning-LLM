from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .unionByName(
        df_sessions
        .filter(F.col("country").isin(["US", "SG", "TH"]))
        .select("country")
    )
    .orderBy("country")
)
