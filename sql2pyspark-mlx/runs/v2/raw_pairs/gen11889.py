from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        F.percent_rank().over(w).alias("revenue_pct_rank"),
    )
)
