from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("sale_date")
w2 = Window.partitionBy("store_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "store_id",
        "sale_date",
        "units",
        F.percent_rank().over(w).alias("sale_date_pct_rank"),
        F.percent_rank().over(w2).alias("units_pct_rank"),
    )
)
