from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([5, 17]))
    .select("browser", "country")
    .unionByName(
        df_sessions
        .filter(F.col("country") != "TH")
        .select("browser", "country")
    )
    .orderBy("country")
)
