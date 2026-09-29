from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id")

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-04-13", "2024-02-29"))
    .select(
        "id",
        "product_id",
        "store_id",
        "sale_date",
        "units",
        F.sum("units").over(w).alias("running_units"),
        F.sum("units").over(w2).alias("store_id_sum_units"),
    )
)
