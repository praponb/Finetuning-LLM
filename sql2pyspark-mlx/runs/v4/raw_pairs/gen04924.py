from pyspark.sql import functions as F

result = df_suppliers.select("city", F.coalesce("city", F.lit("N/A")).alias("city_filled"))
