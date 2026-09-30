from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4]))
    .select(
        "id",
        "store_id",
        "revenue",
        F.percent_rank().over(w).alias("revenue_pct_rank"),
    )
    .orderBy("id")
)
