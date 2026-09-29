from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_4"),
        F.row_number().over(w2).alias("launch_date_row"),
    )
)
