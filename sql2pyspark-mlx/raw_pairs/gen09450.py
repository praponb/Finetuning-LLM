from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc())
w2 = Window.partitionBy("product_id").orderBy("units", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        F.percent_rank().over(w).alias("units_pct_rank"),
        F.row_number().over(w2).alias("row_num"),
    )
)
