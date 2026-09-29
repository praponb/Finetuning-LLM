from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        (F.col("revenue") - F.lag("revenue", 2).over(w)).alias("revenue_change"),
    )
)
