from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_products
    .filter(F.col("cost").isNotNull())
    .select(
        "id",
        "supplier_id",
        "category",
        "launch_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_4"),
    )
    .orderBy(F.col("id").desc(), "launch_date")
)
