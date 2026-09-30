from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        "sale_date",
        F.first("sale_date").over(w).alias("first_sale_date"),
    )
)
