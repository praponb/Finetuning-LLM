from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(260, 380))
    .select("id", "cost")
)
