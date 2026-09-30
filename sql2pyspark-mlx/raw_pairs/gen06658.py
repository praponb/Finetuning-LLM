from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_products
    .groupBy(
        F.when(F.col("category").isin(["Grocery", "Home"]), "tier_1").otherwise("other").alias("category_group"),
    )
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("pct_of_total", F.round(((F.col("total_cost") * 100) / F.sum("total_cost").over(w)), 2))
)
