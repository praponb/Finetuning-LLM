from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 5, 6, 7]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        F.count("*").over(w).alias("store_id_count"),
        F.first("units").over(w2).alias("first_units"),
    )
    .orderBy(F.col("first_units").desc())
)
