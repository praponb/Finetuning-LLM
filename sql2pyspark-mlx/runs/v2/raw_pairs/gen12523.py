from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("revenue").between(3000, 3900))
    .select("id", "product_id", "sale_date", F.ntile(4).over(w).alias("quartile"))
)
