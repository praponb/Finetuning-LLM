from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["New York", "Bangkok"]) & F.col("city").isin(["Chiang Mai", "New York"]))
    .select("id", "opened_date")
    .orderBy(F.col("opened_date").desc(), F.col("id").desc())
    .limit(3)
)
