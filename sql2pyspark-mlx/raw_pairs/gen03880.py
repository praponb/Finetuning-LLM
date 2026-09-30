from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 5, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.avg("units").over(w_ordered).alias("last7_avg_units"),
        F.count("units").over(w_ordered).alias("last7_count_units"),
        F.min("units").over(w_ordered).alias("last7_min_units"),
    )
)
