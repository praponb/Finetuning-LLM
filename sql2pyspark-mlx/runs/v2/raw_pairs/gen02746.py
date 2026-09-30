from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("units").between(22, 29))
    .select(
        "id",
        "product_id",
        "revenue",
        "sale_date",
        "store_id",
        F.first("sale_date").over(w).alias("first_sale_date"),
        F.lead("revenue", 1).over(w2).alias("next_revenue"),
    )
)
