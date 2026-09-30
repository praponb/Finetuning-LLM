from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("quantity", "id")
w3 = Window.orderBy("running_quantity_str", "id").rowsBetween(-5, -1)
w4 = Window.orderBy("running_quantity_str", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("quantity") <= 224)
    .select(
        "id",
        "quantity",
        "last_restock",
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.sum("quantity_str").over(w).alias("running_quantity_str"),
        F.ntile(4).over(w2).alias("quantity_bucket"),
    )
)

with_windows_inventory = (
    with_windows
    .withColumn("prev_quantity_str", F.lag("quantity_str", 1).over(w2))
    .withColumn("quantity_avg_5_prior", F.avg("quantity").over(w3))
    .withColumn("running_running_quantity_str", F.sum("running_quantity_str").over(w4))
)

result = (
    with_windows_inventory
    .groupBy(
        F.when(F.col("prev_quantity_str") >= 76, "HIGH").otherwise("LOW").alias("prev_quantity_str_tier"),
    )
    .agg(
        F.percentile_approx("running_quantity_str", [0.25, 0.5, 0.75]).alias("running_quantity_str_quartiles"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("running_quantity_str", [0.25, 0.5, 0.75]).alias("running_quantity_str_quartiles_2"),
        F.count("*").alias("cnt"),
        F.avg("prev_quantity_str").alias("avg_prev_quantity_str"),
    )
    .orderBy(F.col("cnt").desc())
)
