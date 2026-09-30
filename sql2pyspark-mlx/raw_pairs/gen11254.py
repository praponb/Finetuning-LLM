from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") != "JP") | ~F.col("browser").isin(["Edge", "Safari"]))
    .select("id", "country")
)
