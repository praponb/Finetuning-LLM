from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isNull() & ~F.col("is_active"))
    .select("city", "country")
)
