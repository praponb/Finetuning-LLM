from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("city").isin(["New York", "Berlin", "Bangkok"]))
    .select("city", "country")
    .distinct()
    .orderBy(F.col("city").desc())
)
