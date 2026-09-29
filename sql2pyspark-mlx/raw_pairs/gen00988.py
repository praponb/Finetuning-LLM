from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .subtract(
        df_sessions
        .filter(F.col("country").isin(["JP", "TH"]))
        .select("country")
    )
)
