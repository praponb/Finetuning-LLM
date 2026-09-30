from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.year("signup_date") >= 2024)
    .groupBy("is_active")
    .pivot("city", ["New York", "Bangkok"])
    .agg(F.sum("age"))
)
