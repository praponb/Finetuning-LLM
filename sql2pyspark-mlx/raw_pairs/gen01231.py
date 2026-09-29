from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 6, 9]))
    .select(
        "id",
        "sale_date",
        "revenue",
        "product_id",
        "store_id",
        "units",
        F.round(F.avg("revenue").over(w), 2).alias("revenue_moving_avg_7"),
        F.rank().over(w2).alias("units_rank"),
    )
)
