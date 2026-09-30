from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category")

enriched = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .withColumn("running_count_cost", F.count("cost").over(w))
    .withColumn("running_avg_cost", F.avg("cost").over(w))
    .withColumn("running_max_cost", F.max("cost").over(w))
    .withColumn("category_max_cost", F.max("cost").over(w2))
)

result = (
    enriched
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "tags",
        "running_count_cost",
        "running_avg_cost",
    )
    .orderBy(F.col("id").desc())
)
