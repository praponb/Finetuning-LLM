from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("sale_date"), "store_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "store_id",
        "revenue",
        F.percent_rank().over(w).alias("revenue_pct_rank"),
    )
    .orderBy(F.col("revenue").desc())
)
