from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc())

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        F.percent_rank().over(w).alias("sale_date_pct_rank"),
    )
    .orderBy("sale_date")
)
