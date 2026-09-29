from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([1, 7, 10]))
    .select("id", "tags", "cost", "price")
)
