from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3, 5]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
    .orderBy("store_id")
)
