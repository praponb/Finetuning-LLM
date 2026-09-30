from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(510, 990) & ((F.col("price") <= 1000) | F.month("launch_date").isin([3, 10, 11])))
    .select("id", "cost", F.initcap(F.trim("brand")).alias("brand_title"))
)
