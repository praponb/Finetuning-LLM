from pyspark.sql import functions as F

result = (
    df_products
    .filter(~(F.col("cost").between(210, 720) | (F.col("cost") < 630)))
    .select("id", "launch_date", "tags", "price")
)
