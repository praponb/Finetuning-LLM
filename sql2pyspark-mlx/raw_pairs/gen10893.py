from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.partitionBy("brand", "category").orderBy("price_whole", "id")
w3 = Window.partitionBy("category").orderBy(F.col("price_whole").desc(), "id")

cleaned = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-03-29").cast("date"))
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "launch_date",
        "tags",
        F.ceil("cost").alias("cost_whole"),
        F.ceil("price").alias("price_whole"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "cost_whole",
        "price_whole",
        F.min("cost_whole").over(w).alias("category_min_cost_whole"),
        F.lead("cost", 1, 0).over(w2).alias("next_cost"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
