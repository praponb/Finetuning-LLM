from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.length("email") >= 7) & F.col("name").like("Ca%"))
    .orderBy(F.col("id").desc())
)
