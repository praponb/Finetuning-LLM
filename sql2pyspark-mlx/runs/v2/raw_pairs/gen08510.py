from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3, 4]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "units",
        F.round(F.avg("revenue").over(w), 2).alias("revenue_moving_avg_7"),
        F.percent_rank().over(w2).alias("units_pct_rank"),
    )
)
