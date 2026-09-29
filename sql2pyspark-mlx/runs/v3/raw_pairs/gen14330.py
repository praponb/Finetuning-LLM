from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock_cohort")

result = (
    df_inventory
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(F.sum("quantity").alias("total_quantity"))
    .withColumn("cumulative_quantity", F.sum("total_quantity").over(w))
    .orderBy(F.col("last_restock_cohort").desc())
)
