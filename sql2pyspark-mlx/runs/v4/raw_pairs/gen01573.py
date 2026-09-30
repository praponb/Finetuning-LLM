from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select(
        "id",
        "country",
        "rating",
        "supplier_name",
        F.first("supplier_name").over(w).alias("first_supplier_name"),
    )
    .orderBy("supplier_name")
)
