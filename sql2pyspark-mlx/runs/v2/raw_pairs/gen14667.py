from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        F.percent_rank().over(w).alias("sale_date_pct_rank"),
    )
    .orderBy("id")
)
