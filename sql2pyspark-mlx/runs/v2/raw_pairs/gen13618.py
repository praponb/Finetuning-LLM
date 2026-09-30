from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "revenue",
        "product_id",
        F.lead("revenue", 1).over(w).alias("next_revenue"),
        F.rank().over(w2).alias("rnk"),
    )
)
