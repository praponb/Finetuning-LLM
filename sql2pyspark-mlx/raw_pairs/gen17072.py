from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNull() & ((F.length("name") < 11) | ~F.col("name").like("Ka%")))
    .select("id", "name")
    .orderBy("name", "id")
)
