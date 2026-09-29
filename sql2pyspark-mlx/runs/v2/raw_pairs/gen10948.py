from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "units",
        F.percent_rank().over(w).alias("units_pct_rank"),
    )
)
