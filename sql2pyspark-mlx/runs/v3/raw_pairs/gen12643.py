from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") > 37)
    .select("id", "country")
    .orderBy("id")
)
